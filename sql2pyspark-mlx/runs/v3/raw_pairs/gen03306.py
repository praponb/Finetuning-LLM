from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([2, 5, 10]))
    .groupBy(
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
        F.quarter("sale_date").alias("sale_quarter"),
        "store_id",
    )
    .agg(F.min("sale_date").alias("min_sale_date"))
    .orderBy("store_id")
)
