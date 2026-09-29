from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first(), "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last(), "id")
w3 = Window.partitionBy("dept_id").orderBy("salary")
w_part = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("location").orderBy("seq_no", "id")
w6 = Window.partitionBy("dept_name", "location")
w7 = Window.partitionBy("location").orderBy(F.col("salary").desc(), "id")
w8 = Window.orderBy("salary_dense_rank", "id")
w_recent = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-3, -1)

joined = (
    df_employees
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.email",
        "a.dept_id",
        "a.salary",
        F.col("p.budget"),
        F.col("p.location"),
        F.col("p.dept_name"),
    )
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .filter(F.upper(F.trim("email")) == "DAVID.LEE@GMAIL.COM")
    .withColumn("salary_dense_rank", F.dense_rank().over(w3))
    .withColumn("seq_no", F.row_number().over(w_part))
)

scored = (
    with_windows
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("dept_name_sum_salary_dense_rank", F.sum("salary_dense_rank").over(w6))
    .withColumn("seq_no_2", F.row_number().over(w7))
    .withColumn("salary_dense_rank_bucket", F.ntile(3).over(w8))
    .withColumn("salary_dense_rank_avg_3_prior", F.avg("salary_dense_rank").over(w_recent))
)

result = (
    scored
    .select(
        "id",
        "salary",
        "budget",
        "location",
        "row_num",
        "dedup_rank",
        "salary_dense_rank",
        "seq_no",
        "dept_name_sum_salary_dense_rank",
        F.when(F.col("location").rlike("^(New York|Tokyo)$"), "GROUP_A").when(F.col("location").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("GROUP_B").alias("location_zone"),
    )
)
