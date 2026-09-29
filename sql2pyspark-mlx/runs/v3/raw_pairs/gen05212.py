from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.year("sale_date") == 2023) & F.month("sale_date").isin([4, 6, 10]) & (F.col("units") == 29))
    .withColumn("units_rounded", F.round("units", 0))
)
