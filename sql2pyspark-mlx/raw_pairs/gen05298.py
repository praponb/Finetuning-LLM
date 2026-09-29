from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select("level")
    .unionByName(
        df_orders
        .filter(F.col("discount") > 0.21)
        .select(F.col("channel").alias("level"))
    )
    .distinct()
    .orderBy("level")
)
