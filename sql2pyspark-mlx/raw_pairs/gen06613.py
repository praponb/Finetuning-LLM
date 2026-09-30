from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w_grp = Window.orderBy("budget", "id")
w3 = Window.partitionBy("dept_name")
w4 = Window.orderBy("budget")

base = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Tokyo"]))
    .withColumn("location_dept_name_key", F.concat_ws("/", "location", "dept_name"))
    .withColumn("budget_tier", F.when(F.col("budget") >= 4404000, "High").when(F.col("budget") >= 1448000, "Medium").otherwise("Low"))
)

eligible = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < "2024-10-01"), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.job_title"))
)

eligible_departments = (
    with_last
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.email").like("%@company.co")), "left_semi")
)

metrics = (
    eligible_departments
    .filter(F.col("level").isNotNull())
    .withColumn("prev_budget", F.lag("budget", 1).over(w_grp))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w3)), 4))
    .withColumn("budget_percent_rank", F.percent_rank().over(w4))
)

kept_keys = (
    metrics
    .select("dept_name", "id")
    .intersectAll(
        df_departments
        .filter(F.col("location") != "New York")
        .select("dept_name", "id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_dept_name_key",
        "budget_tier",
        "level",
        "job_title",
        "prev_budget",
    )
    .orderBy(F.col("id").desc())
)
