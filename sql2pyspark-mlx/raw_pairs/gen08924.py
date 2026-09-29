from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

filtered = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("level")).alias("level_title"),
        F.floor("salary").alias("salary_whole"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_filled"),
    )
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
