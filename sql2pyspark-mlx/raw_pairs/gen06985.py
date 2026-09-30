from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("location").eqNullSafe(F.lit("Berlin"))))
    .select(
        "*",
        F.initcap(F.trim("location")).alias("location_title"),
        F.round("budget", 1).alias("budget_rounded"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "dept_name")
)

kept_keys = (
    ranked
    .select("id")
    .intersect(
        df_departments
        .filter(F.col("location") == "Berlin")
        .select("id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("N/A")))
    .withColumn("is_marketing", F.when(F.col("dept_name") == "Marketing", 1).otherwise(0))
    .withColumn("dept_name_filled_2", F.coalesce("dept_name", F.lit("N/A")))
)

result = (
    base
    .groupBy(
        F.when(F.col("is_marketing") >= 68, "HIGH").otherwise("LOW").alias("is_marketing_tier"),
    )
    .agg(
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("is_marketing"))).alias("legal_total_is_marketing"),
        F.max("is_marketing").alias("max_is_marketing"),
        F.avg("is_marketing").alias("avg_is_marketing"),
        F.round(F.avg("is_marketing"), 2).alias("avg_is_marketing_2"),
        F.max("is_marketing").alias("max_is_marketing_2"),
    )
    .orderBy(F.col("max_is_marketing_2").desc())
)
