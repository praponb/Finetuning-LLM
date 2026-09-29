from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Legal"]))
    .agg(
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
    )
    .orderBy(F.col("london_count").desc())
)
