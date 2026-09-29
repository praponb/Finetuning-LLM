from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.countDistinct("units").alias("unique_products"))
    .orderBy("sale_year")
)
