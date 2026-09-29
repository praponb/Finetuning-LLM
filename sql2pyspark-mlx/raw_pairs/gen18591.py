from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("is_jp", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%iam%"))
    .withColumn("is_jp", F.when(F.col("country") == "JP", 1).otherwise(0))
)

result = (
    base
    .filter(F.col("city") != "New York")
    .select(
        "id",
        "country",
        "city",
        "is_jp",
        F.count("rating").over(w_running).alias("running_count_rating"),
        F.avg("rating").over(w_running).alias("running_avg_rating"),
        F.max("rating").over(w_running).alias("running_max_rating"),
    )
)
