from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("product").orderBy(F.col("premium").desc(), F.col("id").desc())

filtered = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) | (F.col("premium") <= 3521.8))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
    )
)

latest_per_key = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .groupingSets(
        [["status", "product", "holder_id"], ["status"]],
        "status", "product", "holder_id",
    )
    .agg(F.array_sort(F.collect_set("product")).alias("product_list"))
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "holder_id",
        "product_list",
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
