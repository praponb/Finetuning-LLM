from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

joined = (
    flattened
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select("e.customer_id", "e.created_at", F.col("p.name"))
)

filtered = (
    joined
    .filter(F.to_date("created_at") >= "2024-12-01")
    .select("*", F.to_date("created_at").alias("created_at_date"))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.email"), F.col("d.city"))
)

summary = (
    with_dims
    .withColumn("activity_month", F.date_trunc("MONTH", "created_at_date"))
    .cube("activity_month")
    .agg(
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    summary
    .select("activity_month", "num_cities", "row_count", "gid")
    .orderBy(F.col("row_count").asc_nulls_first())
)
