from pyspark.sql import functions as F

kept_keys = (
    df_products
    .select("category")
    .intersect(
        df_products
        .filter(F.col("cost") >= 500)
        .select("category")
    )
)

verified = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("WEEK", "launch_date").alias("week_start"))
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("cost")).otherwise(0)).alias("electronics_cost"),
    )
)
