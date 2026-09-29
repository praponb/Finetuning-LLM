from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy("budget", "id")
w4 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_last())
w5 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("budget_share", "id")
w7 = Window.orderBy(F.col("first_known_budget").desc(), "id")
w8 = Window.orderBy("budget_cume_dist", "id")
w9 = Window.orderBy("budget_share", "id").rowsBetween(-2, Window.currentRow)
w10 = Window.orderBy(F.col("row_seq").desc_nulls_first())
w_ordered = Window.orderBy("budget_share_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy(F.col("budget_share_avg_2").desc(), "id")

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_upper", F.upper("dept_name"))
)

enriched = (
    base
    .filter(F.col("location").isin(["London", "Tokyo", "Bangkok"]))
    .select(
        "id",
        "budget",
        "dept_name_upper",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.first("budget", ignorenulls=True).over(w2).alias("first_known_budget"),
        F.row_number().over(w3).alias("row_seq"),
        F.cume_dist().over(w4).alias("budget_cume_dist"),
        F.sum("budget").over(w5).alias("running_budget"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "budget_share",
        "first_known_budget",
        "row_seq",
        "budget_cume_dist",
        F.lag("budget_share", 1).over(w6).alias("prev_budget_share"),
        F.row_number().over(w7).alias("event_seq"),
        F.lag("budget_cume_dist", 1).over(w8).alias("prev_budget_cume_dist"),
        F.avg("budget_share").over(w9).alias("budget_share_avg_2"),
        F.percent_rank().over(w10).alias("row_seq_percent_rank"),
    )
)

enriched_departments = (
    with_windows
    .withColumn("first_known_budget_cume_dist", F.first("budget_cume_dist", ignorenulls=True).over(w_ordered))
    .withColumn("row_seq_2", F.row_number().over(w12))
)

result = (
    enriched_departments
    .select(
        "id",
        "budget_share",
        "first_known_budget",
        "prev_budget_share",
        "prev_budget_cume_dist",
        "budget_share_avg_2",
        "row_seq_percent_rank",
        "first_known_budget_cume_dist",
        "row_seq_2",
    )
    .orderBy(F.col("first_known_budget_cume_dist").asc_nulls_last())
)
