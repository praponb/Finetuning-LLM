from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("country").orderBy("stock_qty", "id")
w_hist = Window.partitionBy("country").orderBy("rating_rounded_int", "id")

cleaned = (
    df_suppliers
    .filter(F.col("country") != "UK")
    .withColumn("rating_rounded", F.round("rating", 1))
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.length(F.col("c.product_name")) > 8), "left_anti")
)

base = (
    eligible
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "rating_rounded",
        "stock_qty",
        F.round((F.col("rating_rounded") / F.col("rating")), 2).alias("rating_rounded_per_rating"),
    )
)

cleaned_suppliers = (
    base
    .filter(F.col("rating_rounded_per_rating") < 68)
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "stock_qty",
        "rating_rounded_per_rating",
        F.col("rating_rounded").cast("int").alias("rating_rounded_int"),
    )
)

with_windows = (
    cleaned_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        "stock_qty",
        "rating_rounded_int",
        F.lead("stock_qty").over(w_ordered).alias("next_stock_qty"),
        F.lag("rating").over(w_ordered).alias("prev_rating"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w_hist).alias("seq_no_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "supplier_name",
        "rating",
        "stock_qty",
        "next_stock_qty",
        "prev_rating",
        "seq_no",
        F.when(F.col("supplier_name").rlike("^(Pacific Wholesale|Tokyo Traders)$"), "OTHER_ZONE").when(F.col("supplier_name").rlike("^(Siam Parts Co|Global Supply)$"), "PRIORITY").otherwise("GROUP_B").alias("supplier_name_zone"),
    )
)
