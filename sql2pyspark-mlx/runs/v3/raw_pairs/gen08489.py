from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("is_remote"))
    .select(
        "id",
        "salary",
        F.regexp_replace("full_name", " ", "-").alias("full_name_slug"),
        F.when(F.col("level").isin(["L2", "L1"]), "Group A").otherwise("Other").alias("level_group"),
    )
    .orderBy(F.col("id").desc(), F.col("salary").desc())
    .limit(10)
)
