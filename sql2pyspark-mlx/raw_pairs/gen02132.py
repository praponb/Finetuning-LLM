from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget_rounded", "id")

cleaned = (
    df_departments
    .filter(F.col("budget") >= 2907000)
    .withColumn("budget_rounded", F.round("budget", 0))
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Sales", "Legal"]), "Tier 1").otherwise("Other"))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

result = (
    cleaned
    .select(
        "id",
        "dept_name",
        "budget",
        "dept_name_group",
        F.last("location", ignorenulls=True).over(w).alias("last_known_location"),
        F.lead("budget_rounded", 2).over(w2).alias("next_budget_rounded"),
    )
)
