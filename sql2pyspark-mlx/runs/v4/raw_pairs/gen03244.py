from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget_filled").desc(), "id")
w2 = Window.orderBy(F.col("adjusted_budget_filled").asc_nulls_first())
w3 = Window.orderBy(F.col("adjusted_budget_filled").desc_nulls_first())

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    latest
    .filter(F.col("rank_in_group") > 1)
    .select(
        "id",
        "budget",
        F.round((F.col("budget_filled") * 1.07), 2).alias("adjusted_budget_filled"),
    )
)

matched_keys = (
    base
    .select("id")
    .exceptAll(
        df_departments
        .filter(~F.col("location").isin(["Chiang Mai", "Bangkok", "New York"]))
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.rank().over(w2).alias("adjusted_budget_filled_rank"),
        F.cume_dist().over(w3).alias("adjusted_budget_filled_cume_dist"),
    )
)

result = (
    enriched
    .select("budget", "adjusted_budget_filled_rank", "adjusted_budget_filled_cume_dist")
)
