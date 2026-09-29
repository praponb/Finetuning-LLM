from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(272, 370) & (F.col("brand") == "Umbrella"))
    .withColumn("brand_length", F.length("brand"))
)
