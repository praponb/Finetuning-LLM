from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("WEEK", "sale_date").alias("week_start"))
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.bit_and("units").alias("units_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
    )
    .filter(F.col("units_bit_and") > 1)
)
