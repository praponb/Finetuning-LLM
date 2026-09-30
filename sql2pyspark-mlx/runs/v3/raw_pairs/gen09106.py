from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("supplier_id") == 9) & F.col("supplier_id").isin([21, 23, 24]) & (F.col("launch_date") < "2024-03-01"))
)

supplier_id_summary = (
    recent_products
    .groupBy(
        "supplier_id",
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.min("cost").alias("min_cost"),
    )
)

result = supplier_id_summary.orderBy(F.col("supplier_id").desc(), "launch_month")
