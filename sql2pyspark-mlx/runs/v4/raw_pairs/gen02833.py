from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget")
w2 = Window.partitionBy("dept_name").orderBy("adjusted_budget", "id")
w3 = Window.orderBy("row_num", "id")
w4 = Window.partitionBy("dept_name").orderBy("adjusted_budget")
w5 = Window.partitionBy("location").orderBy(F.col("budget_filled").desc(), "id")
w6 = Window.partitionBy("dept_name", "location").orderBy("budget_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("location") != "Singapore")
    .select(
        "*",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.substring("location", 1, 4).alias("location_prefix"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id") == 26), "left_semi")
)

latest = (
    kept
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

base = (
    latest
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget_filled",
        "location_prefix",
        "row_num",
        F.round((F.col("budget") * 1.1), 2).alias("adjusted_budget"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.ntile(10).over(w2).alias("adjusted_budget_bucket"),
        F.row_number().over(w3).alias("event_seq"),
        F.percent_rank().over(w4).alias("adjusted_budget_percent_rank"),
        F.row_number().over(w5).alias("row_seq"),
        F.sum("budget_filled").over(w6).alias("running_budget_filled"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "dept_name",
        "location",
        "budget_filled",
        "location_prefix",
        "adjusted_budget",
        "adjusted_budget_bucket",
        "event_seq",
        "row_seq",
        F.when(F.col("location").rlike("^(Tokyo|Bangkok)$"), "CORE").when(F.col("location").rlike("^(Chiang Mai|New York)$"), "EXTENDED").otherwise("GROUP_A").alias("location_zone"),
    )
)
