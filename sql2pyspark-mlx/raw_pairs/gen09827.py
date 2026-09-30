from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "customer_id").orderBy("created_at", "id", "k")
w2 = Window.partitionBy("k").orderBy(F.col("k_length").desc_nulls_last(), "id", "k")

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("attributes").alias("k", "v"))
)

deduped = (
    lines
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "k", "v")
)

base = (
    deduped
    .filter(F.col("k") == "utm_source")
    .withColumn("k_length", F.length("k"))
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "k",
        "k_length",
        "dedup_rank",
        F.when(F.col("k").rlike("^(utm_source|weight_kg)$"), "OTHER_ZONE").when(F.col("k").rlike("^(device|gift)$"), "GROUP_B").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("k").desc())
)
