from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 3, 6]) & F.col("units").isNull())
    .select("id", "store_id", "sale_date", F.round("units", 2).alias("units_rounded"))
    .orderBy(F.col("sale_date").desc(), "id")
    .limit(10)
)
