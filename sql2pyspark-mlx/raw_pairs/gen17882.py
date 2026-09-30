from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location")
w_running = Window.partitionBy("dept_name").orderBy("budget_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("location").orderBy("cumulative_budget_whole")
w5 = Window.partitionBy("location", "dept_name").orderBy("budget_whole", "id")
w6 = Window.partitionBy("location").orderBy("budget_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("budget").between(1875000, 3956000))
    .withColumn("budget_adjusted", F.round((F.col("budget") * 0.85), 2))
    .withColumn("budget_whole", F.ceil("budget"))
)

enriched = (
    cleaned
    .filter(F.col("budget_whole") <= 57)
    .select(
        "*",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.sum("budget_whole").over(w_running).alias("cumulative_budget_whole"),
        F.avg("budget_whole").over(w3).alias("budget_whole_avg_3"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "dept_name",
        "location",
        "budget_adjusted",
        "budget_whole",
        "budget_share",
        "budget_whole_avg_3",
        "row_num",
        F.row_number().over(w5).alias("row_seq"),
        F.sum("budget_adjusted").over(w6).alias("cumulative_budget_adjusted"),
    )
)

result = (
    metrics
    .groupBy("location")
    .pivot("dept_name", ["HR", "Legal", "Marketing"])
    .agg(F.avg("budget_share"))
)
