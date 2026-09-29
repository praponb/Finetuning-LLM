from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 8]))
    .withColumn("sale_date_month", F.month("sale_date"))
)

agg = (
    src
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.max("units").alias("max_units"))
)

result = (
    agg
    .select("sale_year", "max_units")
    .orderBy("max_units")
)
