from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("full_name_length").desc(), "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")
w4 = Window.partitionBy("level").orderBy("salary")
w_running = Window.partitionBy("level").orderBy(F.col("full_name_length").desc_nulls_last())
w_hist = Window.partitionBy("level")
w_ordered = Window.partitionBy(F.year("hire_date"), "level").orderBy("salary")

cleaned = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-04-13").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.length("full_name").alias("full_name_length"),
    )
)

metrics = (
    cleaned
    .withColumn("full_name_length_bucket", F.ntile(3).over(w))
    .withColumn("full_name_length_avg_6", F.avg("full_name_length").over(w2))
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

matched_keys = (
    ranked
    .select("level", "id")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([4, 13, 24]))
        .select("level", "id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "id",
        "email",
        "salary",
        "full_name_length_bucket",
        "full_name_length_avg_6",
        F.rank().over(w4).alias("salary_rank"),
        F.rank().over(w_running).alias("full_name_length_rank"),
        F.avg("full_name_length").over(w_hist).alias("level_avg_full_name_length"),
        F.cume_dist().over(w_ordered).alias("salary_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("full_name_length_rank") >= 7, "LARGE").otherwise("SMALL").alias("full_name_length_rank_tier"),
    )
    .agg(
        F.percentile_approx("full_name_length_rank", 0.9).alias("full_name_length_rank_p90"),
        F.sum("full_name_length_rank").alias("total_full_name_length_rank"),
    )
)
