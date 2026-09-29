from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("units").between(18, 29))

agg = (
    base
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg((F.max("units") - F.min("units")).alias("units_range"))
)

result = (
    agg
    .filter(F.col("units_range") > 46)
    .orderBy("sale_quarter")
)
