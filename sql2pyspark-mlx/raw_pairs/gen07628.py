from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name")
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_last())

base = (
    df_departments
    .filter(F.col("dept_name").isNull())
    .select(
        "*",
        F.concat_ws(",", "dept_name", "location").alias("dept_name_location_key"),
        F.initcap(F.trim("location")).alias("location_title"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

kept_keys = (
    latest
    .select("dept_name")
    .intersectAll(
        df_departments
        .filter(F.col("location").isin(["Bangkok", "New York", "Chiang Mai"]))
        .select("dept_name")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("budget_rank", F.rank().over(w3))
    .withColumn("dept_name_avg_budget", F.avg("budget").over(w2))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.is_remote")), "left_anti")
)

result = (
    not_flagged
    .groupBy("location")
    .pivot("dept_name", ["Marketing", "Legal"])
    .agg(F.sum("budget"))
)
