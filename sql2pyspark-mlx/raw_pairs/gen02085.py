from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id", "level").orderBy(F.col("employee_salary").asc_nulls_last())
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("manager_id").orderBy("salary", "id")
w6 = Window.partitionBy("level").orderBy(F.col("salary").desc())

scored = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "is_remote",
        F.lead("salary").over(w_part).alias("next_salary"),
        F.lag("salary").over(w_part).alias("prev_salary"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .select(
        "src.id",
        "src.email",
        "src.manager_id",
        "src.is_remote",
        "src.row_seq",
        F.col("e.salary"),
        F.col("e.level"),
        F.col("e.full_name").alias("employee_full_name"),
    )
)

enriched_employees = (
    enriched
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.salary").alias("employee_salary"))
)

ranked = (
    enriched_employees
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "manager_id", "row_seq", "salary", "level")
)

metrics = (
    ranked
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.avg("salary").over(w4).alias("salary_avg_5"),
    )
)

scored_employees = (
    metrics
    .select(
        "id",
        "salary",
        "salary_avg_5",
        F.lead("salary_share").over(w_hist).alias("next_salary_share"),
        F.lag("salary_share").over(w_hist).alias("prev_salary_share"),
        F.lag("row_seq").over(w_hist).alias("prev_row_seq"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.cume_dist().over(w6).alias("salary_cume_dist"),
    )
)

result = (
    scored_employees
    .select(
        "id",
        F.col("salary_cume_dist").alias("SALARY_CUME_DIST"),
        F.col("next_salary_share").alias("NEXT_SALARY_SHARE_VAL"),
        "salary",
        F.col("prev_salary_share").alias("PREV_SALARY_SHARE"),
    )
    .unpivot(
        ids=["id"],
        values=["SALARY_CUME_DIST", "NEXT_SALARY_SHARE_VAL", "salary", "PREV_SALARY_SHARE"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
