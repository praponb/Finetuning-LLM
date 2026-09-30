from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("dept_id", "level").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("dept_id")
w4 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id")
w6 = Window.partitionBy("dept_id").orderBy("salary", "id")
w7 = Window.partitionBy("level", "manager_id").orderBy(F.col("dept_id_stddev_samp_salary").desc())
w8 = Window.partitionBy("manager_id").orderBy(F.col("dept_id_stddev_samp_salary_percent_rank").asc_nulls_first())
w9 = Window.partitionBy("manager_id").orderBy(F.col("dept_id_stddev_samp_salary").desc_nulls_last(), "id")
w10 = Window.partitionBy("job_title").orderBy("salary_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("level")
w13 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w14 = Window.orderBy(F.col("running_dept_id_stddev_samp_salary").desc_nulls_last())

ranked = (
    df_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    ranked
    .select(
        "*",
        F.rank().over(w2).alias("salary_rank"),
        F.stddev_samp("salary").over(w3).alias("dept_id_stddev_samp_salary"),
        F.first("level", ignorenulls=True).over(w4).alias("first_known_level"),
        F.lead("salary", 1).over(w5).alias("next_salary"),
    )
)

enriched_employees = (
    enriched
    .withColumn("salary_bucket", F.ntile(4).over(w6))
    .withColumn("dept_id_stddev_samp_salary_percent_rank", F.percent_rank().over(w7))
)

deduped = (
    enriched_employees
    .withColumn("dedup_rank", F.rank().over(w8))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest = (
    deduped
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_employees_2 = (
    latest
    .select(
        "*",
        F.sum("salary_bucket").over(w10).alias("running_salary_bucket"),
        F.sum("dept_id_stddev_samp_salary").over(w11).alias("running_dept_id_stddev_samp_salary"),
        F.max("salary_rank").over(w12).alias("level_max_salary_rank"),
    )
)

scored = (
    enriched_employees_2
    .select(
        "*",
        F.ntile(5).over(w13).alias("salary_bucket_2"),
        F.cume_dist().over(w14).alias("running_dept_id_stddev_samp_salary_cume_dist"),
    )
)

result = (
    scored
    .select(
        "id",
        "manager_id",
        F.col("running_dept_id_stddev_samp_salary_cume_dist").alias("RUNNING_DEPT_ID_STDDEV_SAMP_SALARY_CUME_DIST_VAL"),
        F.col("dept_id_stddev_samp_salary_percent_rank").alias("DEPT_ID_STDDEV_SAMP_SALARY_PERCENT_RANK_VAL"),
        F.col("next_salary").alias("NEXT_SALARY_VAL"),
    )
    .unpivot(
        ["id", "manager_id"],
        ["RUNNING_DEPT_ID_STDDEV_SAMP_SALARY_CUME_DIST_VAL", "DEPT_ID_STDDEV_SAMP_SALARY_PERCENT_RANK_VAL", "NEXT_SALARY_VAL"],
        "metric_name",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
