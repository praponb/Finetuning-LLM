from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .filter(F.col("unique_stores") < 6)
    .select("store_id", "unique_stores", "last_sale_date")
)
