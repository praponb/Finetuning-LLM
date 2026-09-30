from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("To%"))
    .groupBy("city")
    .agg(F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"))
    .filter(F.col("sg_count") > 49)
    .select("city", "sg_count")
    .orderBy(F.col("sg_count").desc())
)
