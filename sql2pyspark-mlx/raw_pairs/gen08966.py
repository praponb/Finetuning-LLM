from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([2, 4, 11]) & (F.col("product_id") == 7))
    .withColumn("greatest_units_revenue", F.greatest("units", "revenue"))
    .withColumn("sale_date_weekofyear", F.weekofyear("sale_date"))
    .orderBy("sale_date")
)
