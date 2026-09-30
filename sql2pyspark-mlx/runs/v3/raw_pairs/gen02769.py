from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.quarter("sale_date").alias("sale_quarter"),
        F.year("sale_date").alias("sale_year"),
        "store_id",
    )
    .agg(F.max("revenue").alias("max_revenue"))
)
