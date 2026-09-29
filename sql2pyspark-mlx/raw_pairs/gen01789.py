from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("dept_name").orderBy(F.col("salary").asc_nulls_last(), "id")

base = (
    df_departments
    .filter(F.col("budget") >= 3821000)
    .select("*", F.initcap(F.trim("dept_name")).alias("dept_name_title"))
)

with_last = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.salary"))
)

deduped = (
    with_last
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level") == "L2"), "left_semi")
)

latest = (
    kept
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy("dept_name")
    .pivot("level", ["L3", "L2"])
    .agg(F.avg("budget"))
    .select("dept_name", F.col("L3").alias("l3"), F.col("L2").alias("l2"))
)
