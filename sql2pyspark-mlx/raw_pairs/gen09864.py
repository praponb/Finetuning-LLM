from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id")
w_recent = Window.orderBy("salary_adjusted", "id")
w3 = Window.partitionBy(F.year("hire_date"), "level").orderBy(F.col("salary_adjusted").desc(), "id")
w4 = Window.partitionBy("job_title", "level").orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("job_title").orderBy(F.col("salary_int").desc())
w6 = Window.partitionBy("job_title").orderBy("next_salary_adjusted", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("level")

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-02-05").cast("date")) & ~F.col("level").isin(["L5", "L2", "L3"]))
    .select(
        "*",
        F.col("salary").cast("int").alias("salary_int"),
        F.round((F.col("salary") * 1.15), 2).alias("salary_adjusted"),
    )
)

scored = (
    valid
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("next_salary_adjusted", F.lead("salary_adjusted", 1).over(w_recent))
)

with_windows = (
    scored
    .select(
        "id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "salary_int",
        "next_salary_adjusted",
        F.ntile(4).over(w3).alias("salary_adjusted_bucket"),
        F.ntile(10).over(w4).alias("salary_bucket"),
    )
)

scored_employees = (
    with_windows
    .select(
        "id",
        "level",
        "next_salary_adjusted",
        "salary_adjusted_bucket",
        F.rank().over(w5).alias("salary_int_rank"),
        F.avg("salary").over(w6).alias("salary_avg_3_prior"),
        F.sum("next_salary_adjusted").over(w7).alias("level_sum_next_salary_adjusted"),
    )
)

result = (
    scored_employees
    .select(
        "id",
        "level",
        "salary_adjusted_bucket",
        "salary_int_rank",
        F.when(F.col("level").rlike("^(L3|L2)$"), "PRIORITY").otherwise("GROUP_A").alias("level_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last(), "salary_adjusted_bucket")
)
