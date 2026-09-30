from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

deduped = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .withColumn("salary_tier", F.when(F.col("salary") >= 122000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["salary_tier", "level"], ["salary_tier"], []],
        "salary_tier",
        "level",
    )
    .agg(
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.stddev_samp("salary").alias("salary_stddev_2"),
        F.grouping("salary_tier").alias("grouping_id"),
    )
    .filter(F.col("salary_stddev") >= 0)
    .orderBy(F.col("salary_tier").desc())
)
