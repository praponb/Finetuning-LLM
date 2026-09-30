from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("dept_name").eqNullSafe(F.lit("Legal"))))
    .select(
        "*",
        F.when(F.col("dept_name").isin(["Finance", "Legal"]), "Core").otherwise("Other").alias("dept_name_group"),
        F.coalesce("location", F.lit("OTHER")).alias("resolved_location"),
    )
)

result = (
    base
    .groupBy("location")
    .pivot("dept_name", ["Sales", "Marketing"])
    .agg(F.avg("budget").alias("amount"))
    .select(
        "location",
        F.col("Sales").alias("sales"),
        F.col("Marketing").alias("marketing"),
    )
)
