from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("manager_id")
w3 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("level").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("event_seq").desc(), "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-06-30"))) & ~F.col("job_title").isin(["Manager", "Engineer"]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.concat_ws("/", "email", "level").alias("email_level_key"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .filter(F.col("salary") < 128000)
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.row_number().over(w3).alias("event_seq"),
        F.last("level", ignorenulls=True).over(w4).alias("last_known_level"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "level",
        "latest_rank",
        "salary_share",
        "event_seq",
        "last_known_level",
    )
    .orderBy("level", "latest_rank")
)
