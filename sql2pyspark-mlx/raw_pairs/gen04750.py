from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

joined = (
    lines
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.qty",
        F.col("d.name"),
        F.col("d.signup_date"),
        F.col("d.country"),
    )
)

enriched = (
    joined
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select("s.customer_id", "s.qty", "s.name", "s.country", F.col("d.email"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.customer_id", "a.name", "a.email", F.col("p.city"))
)

result = (
    with_dims
    .groupBy("city")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
    )
)
