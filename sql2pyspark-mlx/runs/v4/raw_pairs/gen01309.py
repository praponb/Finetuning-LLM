from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("country_length", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("city").orderBy("country_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("country_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("country_length").desc_nulls_last())
w5 = Window.partitionBy("city").orderBy("country_length", "id")

cleaned = (
    df_suppliers
    .filter(F.col("rating") <= 4.2)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.when(F.col("rating") >= 4.6, "High").when(F.col("rating") >= 1.5, "Medium").otherwise("Low").alias("rating_tier"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

valid = (
    cleaned
    .filter(~F.col("city").isin(["Bangkok", "London", "New York"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.length("country").alias("country_length"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.avg("country_length").over(w).alias("country_length_avg_2_prior"),
        F.sum("country_length").over(w2).alias("running_country_length"),
        F.sum("country_length").over(w3).alias("cumulative_country_length"),
        F.cume_dist().over(w4).alias("country_length_cume_dist"),
        F.ntile(3).over(w5).alias("country_length_bucket"),
    )
)

valid_suppliers = (
    with_windows
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.least("running_country_length", "country_length_cume_dist").alias("least_running_country_length_country_length_cume_dist"),
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
    )
)

result = (
    valid_suppliers
    .select(
        "id",
        "country",
        "running_country_length",
        "country_length_cume_dist",
        "country_length_bucket",
        "least_running_country_length_country_length_cume_dist",
        "supplier_name_normalized",
        F.when(F.col("country").rlike("^(DE|UK)$"), "GROUP_B").when(F.col("country").rlike("^(TH|US)$"), "EXTENDED").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy(
        F.col("least_running_country_length_country_length_cume_dist").asc_nulls_first(),
    )
)
