from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy(F.col("hire_date_period").desc_nulls_first(), "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("hire_date_period").desc(), "id")
w3 = Window.orderBy("salary", "id")
w4 = Window.partitionBy("manager_id").orderBy("hire_date_period", "id")
w5 = Window.partitionBy("manager_id").orderBy("hire_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.date_trunc("YEAR", "hire_date").alias("hire_date_period"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("salary_bucket", F.ntile(3).over(w3))
)

enriched = (
    scored
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.location"), F.col("p.budget"))
)

flagged = (
    enriched
    .withColumn("is_new_burst", F.when(F.lag("hire_date_period").over(w4).isNull() | (F.col("hire_date_period") > F.lag("hire_date_period").over(w4) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_burst").over(w5))

result = (
    islands
    .select("id", "location", "salary", "budget")
    .unpivot(["id", "location"], ["salary", "budget"], "kpi", "reading")
    .filter(F.col("reading").isNotNull())
)
