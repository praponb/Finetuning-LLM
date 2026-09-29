from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .withColumn("units_rounded", F.round("units", 2))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM-dd"))
)
