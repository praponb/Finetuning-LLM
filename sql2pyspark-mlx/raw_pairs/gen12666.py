from pyspark.sql import functions as F

result = (
    df_store_sales
    .rollup("store_id")
    .agg(
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.grouping("store_id").alias("grouping_level"),
    )
    .orderBy(F.col("corr_units_revenue").desc_nulls_last())
)
