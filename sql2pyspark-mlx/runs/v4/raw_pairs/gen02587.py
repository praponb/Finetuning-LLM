from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title", "level")
w3 = Window.orderBy("salary", "id")
w4 = Window.orderBy(F.col("salary").desc())
w5 = Window.partitionBy("job_title")
w6 = Window.orderBy("salary")
w7 = Window.partitionBy("dept_id").orderBy("salary")
w8 = Window.orderBy("salary_share")
w9 = Window.partitionBy("level").orderBy(F.col("next_salary").desc_nulls_first())
w_ordered = Window.partitionBy("level").orderBy(F.col("salary_share_2").desc_nulls_first())

metrics = (
    df_employees
    .select(
        "*",
        F.lead("salary", 2, 0).over(w).alias("next_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

matched_keys = (
    metrics
    .select("job_title")
    .exceptAll(
        df_employees
        .filter(F.col("full_name").isNotNull())
        .select("job_title")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("salary_cume_dist", F.cume_dist().over(w4))
    .withColumn("salary_share_2", F.round((F.col("salary") / F.sum("salary").over(w5)), 4))
    .withColumn("salary_percent_rank", F.percent_rank().over(w6))
    .withColumn("salary_rank", F.rank().over(w7))
    .withColumn("salary_share_percent_rank", F.percent_rank().over(w8))
)

result = (
    with_windows
    .withColumn("next_salary_dense_rank", F.dense_rank().over(w9))
    .withColumn("salary_share_2_rank", F.rank().over(w_ordered))
)
