from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNull())
    .groupBy("is_remote")
    .agg(F.countDistinct("salary").alias("unique_levels"))
    .filter(F.col("unique_levels") >= 5)
    .select("is_remote", "unique_levels")
    .orderBy(F.col("unique_levels").desc())
)
