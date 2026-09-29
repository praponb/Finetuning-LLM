from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") != "Sports"), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "bestseller")), "left_anti")
)

kept_suppliers = (
    eligible
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand") == "Stark"), "left_anti")
)

kept_suppliers_2 = (
    kept_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty") != 144), "left_semi")
)

deduped = (
    kept_suppliers_2
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Grocery", "Sports"])), "left_semi")
)

cleaned = (
    not_flagged
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "row_num",
        F.concat_ws("/", "supplier_name", "country").alias("supplier_name_country_key"),
        F.length("supplier_name").alias("supplier_name_length"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "supplier_name",
        "country",
        "row_num",
        "supplier_name_country_key",
        "supplier_name_length",
        F.when(F.col("country").rlike("^(UK|TH)$"), "CORE").when(F.col("country").rlike("^(US|SG)$"), "EXTENDED").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("id").desc(), "supplier_name_length")
)
