from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title").orderBy(F.col("salary").desc(), "id")

ranked = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy("job_title")
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
    )
    .orderBy(F.col("num_levels").desc_nulls_first())
)
