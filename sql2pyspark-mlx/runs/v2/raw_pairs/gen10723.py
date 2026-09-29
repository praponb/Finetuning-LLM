from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2300, 3100) & (F.col("units") > 40))
    .select("id", "units", F.coalesce("revenue", F.lit(0)).alias("revenue_filled"))
)
