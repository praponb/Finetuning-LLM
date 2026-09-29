from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@yahoo.com"))
    .withColumn("level_full_name", F.concat("level", F.lit(" "), "full_name"))
    .withColumn("email_title", F.initcap(F.trim("email")))
    .orderBy(F.col("manager_id").desc())
)
