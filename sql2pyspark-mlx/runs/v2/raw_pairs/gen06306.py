from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L2", "L5"]) | F.col("email").like("%@gmail.com"))
    .withColumn("level_group", F.when(F.col("level").isin(["L5", "L1"]), "Tier 1").otherwise("Other"))
    .orderBy("salary", "dept_id", "id")
    .limit(20)
)
