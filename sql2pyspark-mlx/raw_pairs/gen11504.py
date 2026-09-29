from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units_adjusted_adjusted").desc(), "id")
w_grp = Window.partitionBy("product_id", "store_id").orderBy("row_num", "id")
w3 = Window.partitionBy("store_id").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-09-29").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.round((F.col("units") * 1.15), 2).alias("units_adjusted"),
        F.when(F.col("units") >= 42, "High").when(F.col("units") >= 13, "Medium").otherwise("Low").alias("units_band"),
    )
)

valid = (
    cleaned
    .filter(F.col("sale_date") >= "2024-06-01")
    .withColumn("units_adjusted_adjusted", F.round((F.col("units_adjusted") * 100), 2))
    .withColumn("units_times_units_adjusted", (F.col("units") * F.col("units_adjusted")))
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

filtered = (
    deduped
    .filter(F.col("sale_date") >= F.lit("2023-03-01").cast("date"))
    .withColumn("greatest_units_adjusted_adjusted_units", F.greatest("units_adjusted_adjusted", "units"))
)

result = (
    filtered
    .filter(F.col("units_adjusted_adjusted").between(31, 54))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units_band",
        "units_adjusted_adjusted",
        "units_times_units_adjusted",
        "row_num",
        F.lag("revenue").over(w_grp).alias("prev_revenue"),
        F.lag("units_adjusted_adjusted").over(w_grp).alias("prev_units_adjusted_adjusted"),
        F.lead("units_adjusted_adjusted").over(w_grp).alias("next_units_adjusted_adjusted"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.first("units", ignorenulls=True).over(w3).alias("first_known_units"),
    )
)
