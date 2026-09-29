from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

metrics = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.rank().over(w2).alias("val_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("val").between(26, 41))
    .select(
        "plan",
        "effective_from",
        "val",
        "val_dense_rank",
        F.upper("plan").alias("plan_upper"),
    )
)

kept_keys = (
    valid
    .select("plan")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("plan")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "plan",
        "effective_from",
        "val_dense_rank",
        "plan_upper",
        F.when(F.col("plan").rlike("^(pro|starter)$"), "EXTENDED").when(F.col("plan").rlike("^(basic|enterprise)$"), "CORE").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy(F.col("val_dense_rank").desc(), "plan")
)
