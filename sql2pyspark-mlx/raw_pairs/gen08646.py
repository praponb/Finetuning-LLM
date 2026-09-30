from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w_part = Window.orderBy(F.col("rating_adjusted").desc(), "id")
w4 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("London")))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.round((F.col("rating") * 1.07), 2).alias("rating_adjusted"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

result = (
    valid
    .filter(F.col("country") == "JP")
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("last_known_rating_adjusted", F.last("rating_adjusted", ignorenulls=True).over(w4))
)
