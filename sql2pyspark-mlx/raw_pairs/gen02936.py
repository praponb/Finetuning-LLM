from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("location_prefix", F.substring("location", 1, 4))
    .withColumn("is_berlin", F.when(F.col("location") == "Berlin", 1).otherwise(0))
)

eligible = (
    valid
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([10, 19])), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", "s.is_berlin", F.col("c.salary"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") < 62000), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([22, 38, 40])), "left_anti")
)

result = (
    kept
    .groupBy("dept_name")
    .agg(
        F.round(F.avg("is_berlin"), 2).alias("avg_is_berlin"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_rows"),
    )
    .orderBy(F.col("sales_rows").desc())
)
