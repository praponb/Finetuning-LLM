from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Marketing"]) & (F.col("budget") > 3828000))
    .withColumn("budget_whole", F.floor("budget"))
    .withColumn("location_group", F.when(F.col("location").isin(["Chiang Mai", "Bangkok"]), "Group A").otherwise("Other"))
    .orderBy("location", F.col("id").desc())
)
