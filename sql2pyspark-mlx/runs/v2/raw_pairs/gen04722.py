from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 61000)
    .select(
        "is_remote",
        F.when(F.col("level") == "L4", 1).otherwise(0).alias("is_l4"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
