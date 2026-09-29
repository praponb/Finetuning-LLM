from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.partitionBy("level").orderBy(F.col("row_seq_plus_prev_salary").desc(), "id")
w4 = Window.partitionBy("level").orderBy("row_seq", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-06-20").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.when(F.col("level").isin(["L5", "L2"]), "Core").otherwise("Other").alias("level_group"),
        F.coalesce("level", F.lit("NONE")).alias("resolved_level"),
    )
)

enriched = (
    filtered
    .filter(F.col("level") == "L1")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "hire_date",
        "level",
        "level_group",
        "resolved_level",
        F.lag("salary", 2).over(w).alias("prev_salary"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

base = (
    enriched
    .filter(F.col("level").eqNullSafe(F.lit("L2")))
    .withColumn("level_group_2", F.when(F.col("level").isin(["L4", "L3"]), "Tier 1").otherwise("Other"))
    .withColumn("row_seq_plus_prev_salary", (F.col("row_seq") + F.col("prev_salary")))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "full_name",
        "dept_id",
        "hire_date",
        "level",
        "level_group",
        "resolved_level",
        "row_seq",
        "level_group_2",
    )
)

scored = (
    latest
    .withColumn("last3_min_row_seq", F.min("row_seq").over(w4))
    .withColumn("last3_avg_row_seq", F.avg("row_seq").over(w4))
    .withColumn("last3_sum_row_seq", F.sum("row_seq").over(w4))
    .withColumn("last3_count_row_seq", F.count("row_seq").over(w4))
)

kept_keys = (
    scored
    .select("level", "id")
    .exceptAll(
        df_employees
        .filter(F.col("email").isNull())
        .select("level", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("dept_id")
    .pivot("level", ["L2", "L4"])
    .agg(F.max("row_seq").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
