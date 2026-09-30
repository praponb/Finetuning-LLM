from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-09-13"))) & F.col("browser").isNotNull())
    .withColumn("clean_country", F.trim("country"))
    .withColumn("started_at_label", F.date_format("started_at", "yyyy-MM"))
    .withColumn("country_clean", F.coalesce("country", F.lit("UNKNOWN")))
)

result = (
    valid
    .select(
        "*",
        F.first("browser", ignorenulls=True).over(w_grp).alias("first_known_browser"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.first("browser", ignorenulls=True).over(w3).alias("first_known_browser_2"),
    )
)
