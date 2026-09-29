from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("units").between(17, 28) & (F.col("store_id").isin([1, 3, 5, 6]) | (F.col("units") > 39)))
)

summary = (
    filtered_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.sum("units").alias("total_units"))
)

result = (
    summary
    .filter(F.col("total_units") < 210)
    .orderBy(F.col("total_units").desc())
)
