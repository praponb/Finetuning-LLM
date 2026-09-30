from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w_part = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("location") == "Singapore"))
    .select("*", F.substring("location", 1, 4).alias("location_prefix"))
)

filtered = (
    valid
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "location",
        "budget",
        "location_prefix",
        F.concat("dept_name", F.lit(" "), "location").alias("dept_name_location"),
        F.initcap(F.trim("location")).alias("location_title"),
    )
)

with_windows = (
    filtered
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w_part))
    .withColumn("last_known_location", F.last("location", ignorenulls=True).over(w_part))
)

result = (
    with_windows
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.size(F.collect_set("location")).alias("num_locations"),
    )
    .orderBy(F.col("seq_no").desc())
)
