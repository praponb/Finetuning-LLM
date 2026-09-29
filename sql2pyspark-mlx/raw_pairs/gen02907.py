from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").asc_nulls_last(), "id")

latest = (
    df_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "row_num",
        F.when(F.col("job_title").rlike("^(Analyst|Director)$"), "OTHER_ZONE").when(F.col("job_title").rlike("^(Engineer|Manager)$"), "GROUP_B").otherwise("EXTENDED").alias("job_title_zone"),
    )
    .orderBy("id")
)
