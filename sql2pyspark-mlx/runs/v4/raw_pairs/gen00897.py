from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_first())
w5 = Window.partitionBy("dept_name").orderBy("seq_no", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first(), "id")
w7 = Window.partitionBy("location").orderBy(F.col("budget_cume_dist").desc(), "id")
w8 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("budget_cume_dist_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("dept_name").orderBy("budget_cume_dist", "id").rowsBetween(-3, -1)
w11 = Window.partitionBy("location").orderBy("budget_cume_dist_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_departments
    .withColumn("last_known_location", F.last("location", ignorenulls=True).over(w_grp))
    .withColumn("last_known_dept_name", F.last("dept_name", ignorenulls=True).over(w2))
)

with_windows_departments = (
    with_windows
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.cume_dist().over(w4).alias("budget_cume_dist"),
        F.first("location", ignorenulls=True).over(w2).alias("first_known_location"),
    )
)

with_windows_departments_2 = (
    with_windows_departments
    .select(
        "*",
        F.first("seq_no", ignorenulls=True).over(w_grp).alias("first_known_seq_no"),
        F.avg("budget_cume_dist").over(w5).alias("budget_cume_dist_avg_5_prior"),
    )
)

ranked = (
    with_windows_departments_2
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.ntile(5).over(w7).alias("budget_cume_dist_bucket"),
        F.sum("seq_no").over(w8).alias("cumulative_seq_no"),
        F.last("location", ignorenulls=True).over(w9).alias("last_known_location_2"),
        F.avg("first_known_seq_no").over(w10).alias("first_known_seq_no_avg_3_prior"),
        F.last("location", ignorenulls=True).over(w11).alias("last_known_location_3"),
    )
)

cleaned = (
    scored
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.concat("location", F.lit("/"), "dept_name").alias("location_dept_name"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("resolved_dept_name"),
    )
)

filtered = (
    cleaned
    .filter(F.col("dept_name").isNotNull() & ~F.col("dept_name").isin(["Finance", "Engineering", "HR"]))
    .select(
        "id",
        "dept_name",
        "location",
        "last_known_location",
        "seq_no",
        "budget_cume_dist",
        "first_known_location",
        "first_known_seq_no",
        "budget_cume_dist_bucket",
        "cumulative_seq_no",
        "last_known_location_2",
        "last_known_location_3",
        "location_dept_name",
        "resolved_dept_name",
        F.ceil("cumulative_seq_no").alias("cumulative_seq_no_whole"),
    )
)

result = (
    filtered
    .select(
        "id",
        "dept_name",
        "location",
        "last_known_location",
        "budget_cume_dist",
        "first_known_location",
        "first_known_seq_no",
        "budget_cume_dist_bucket",
        "last_known_location_2",
    )
    .orderBy(F.col("location").asc_nulls_first())
)
