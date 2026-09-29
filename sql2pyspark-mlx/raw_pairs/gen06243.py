from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("salary_int", F.col("salary").cast("int"))
    .withColumn("full_name_level", F.concat("full_name", F.lit(" "), "level"))
    .orderBy("level", "id")
)
