from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.orderBy("budget", "id")
w3 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("dept_name").isNull())
    .withColumn("dept_name_upper", F.upper("dept_name"))
    .withColumn("clean_location", F.trim("location"))
)

result = (
    base
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_upper",
        "clean_location",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.lead("budget", 2).over(w2).alias("next_budget"),
        F.sum("budget").over(w3).alias("cumulative_budget"),
    )
)
