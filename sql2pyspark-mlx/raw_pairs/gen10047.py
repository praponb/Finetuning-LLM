from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_level", "dept_name").orderBy("location_length", "id")
w3 = Window.partitionBy("employee_level", "location").orderBy(F.col("budget_whole").desc_nulls_last(), "id")

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("location") == "London"))
    .withColumn("location_length", F.length("location"))
    .withColumn("budget_whole", F.ceil("budget"))
)

latest_match = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"), F.col("c.level").alias("employee_level"))
)

latest = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

latest_match_departments = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.location_length",
        "s.budget_whole",
        "s.hire_date",
        "s.employee_level",
        "s.dedup_rank",
        F.col("c.salary"),
    )
)

matched_keys = (
    latest_match_departments
    .select("id", "location")
    .subtract(
        df_departments
        .filter(F.col("budget") >= 2169000)
        .select("id", "location")
    )
)

kept_rows = (
    latest_match_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.level") != "L4"), "left_semi")
)

result = (
    kept
    .select("id", "location_length", "hire_date", "dedup_rank")
    .orderBy(F.col("location_length").desc(), "id")
)
