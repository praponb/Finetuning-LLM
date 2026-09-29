from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_last(), "id")
w2 = Window.partitionBy("dept_name").orderBy("dedup_rank", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("location").orderBy("budget", "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("location_dept_name_key", F.concat_ws("/", "location", "dept_name"))
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["HR", "Finance"]), "Group A").otherwise("Other"))
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .filter(F.col("budget") <= 3623000)
    .select(
        "*",
        F.avg("budget").over(w2).alias("budget_avg_3_prior"),
        F.ntile(10).over(w3).alias("budget_bucket"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("budget") > 1110000)
    .withColumn("budget_avg_3_prior_filled", F.coalesce("budget_avg_3_prior", F.lit(0)))
)

kept_keys = (
    cleaned
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(~F.col("dept_name").isin(["Sales", "Engineering"]))
        .select("dept_name", "location")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "dept_name",
        "location",
        "location_dept_name_key",
        "dept_name_group",
        "dedup_rank",
        "budget_avg_3_prior",
        "budget_bucket",
        "budget_avg_3_prior_filled",
        F.when(F.col("dept_name").rlike("^(Engineering|Marketing)$"), "GROUP_B").when(F.col("dept_name").rlike("^(Sales|HR)$"), "CORE").otherwise("PRIORITY").alias("dept_name_zone"),
    )
)
